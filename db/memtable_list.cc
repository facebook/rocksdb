//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "db/memtable_list.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <string>
#include "rocksdb/db.h"
#include "db/memtable.h"
#include "db/version_set.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "table/merger.h"
#include "util/coding.h"
#include "util/log_buffer.h"
#include "util/thread_status_util.h"

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
                              Status* s, MergeContext* merge_context) {
  for (auto& memtable : memlist_) {
    if (memtable->Get(key, value, s, merge_context)) {
      return true;
    }
  }
  return false;
}

void MemTableListVersion::AddIterators(const ReadOptions& options,
                                       std::vector<Iterator*>* iterator_list,
                                       Arena* arena) {
  for (auto& m : memlist_) {
    iterator_list->push_back(m->NewIterator(options, arena));
  }
}

void MemTableListVersion::AddIterators(
    const ReadOptions& options, MergeIteratorBuilder* merge_iter_builder) {
  for (auto& m : memlist_) {
    merge_iter_builder->AddIterator(
        m->NewIterator(options, merge_iter_builder->GetArena()));
  }
}

uint64_t MemTableListVersion::GetTotalNumEntries() const {
  uint64_t total_num = 0;
  for (auto& m : memlist_) {
    total_num += m->num_entries();
  }
  return total_num;
}

uint64_t MemTableListVersion::GetTotalNumDeletes() const {
  uint64_t total_num = 0;
  for (auto& m : memlist_) {
    total_num += m->num_deletes();
  }
  return total_num;
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
    assert(imm_flush_needed.load(std::memory_order_relaxed));
    return true;
  }
  return false;
}

// Returns the memtables that need to be flushed.
void MemTableList::PickMemtablesToFlush(autovector<MemTable*>* ret) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_PICK_MEMTABLES_TO_FLUSH);
  const auto& memlist = current_->memlist_;
  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
    MemTable* m = *it;
    if (!m->flush_in_progress_) {
      assert(!m->flush_completed_);
      num_flush_not_started_--;
      if (num_flush_not_started_ == 0) {
        imm_flush_needed.store(false, std::memory_order_release);
      }
      m->flush_in_progress_ = true;  // flushing will start very soon
      ret->push_back(m);
    }
  }
  flush_requested_ = false;  // start-flush request is complete
}

void MemTableList::RollbackMemtableFlush(const autovector<MemTable*>& mems,
                                         uint64_t file_number) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_MEMTABLE_ROLLBACK);
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
  imm_flush_needed.store(true, std::memory_order_release);
}

// Record a successful flush in the manifest file
Status MemTableList::InstallMemtableFlushResults(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const autovector<MemTable*>& mems, VersionSet* vset, InstrumentedMutex* mu,
    uint64_t file_number, autovector<MemTable*>* to_delete,
    Directory* db_directory, LogBuffer* log_buffer) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS);
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

    LogToBuffer(log_buffer, "[%s] Level-0 commit table #%" PRIu64 " started",
                cfd->GetName().c_str(), m->file_number_);

    // this can release and reacquire the mutex.
    s = vset->LogAndApply(cfd, mutable_cf_options, &m->edit_, mu, db_directory);

    // we will be changing the version in the next code path,
    // so we better create a new one, since versions are immutable
    InstallNewVersion();

    // All the later memtables that have the same filenum
    // are part of the same batch. They can be committed now.
    uint64_t mem_id = 1;  // how many memtables has been flushed.
    do {
      if (s.ok()) { // commit new state
        LogToBuffer(log_buffer, "[%s] Level-0 commit table #%" PRIu64
                                ": memtable #%" PRIu64 " done",
                    cfd->GetName().c_str(), m->file_number_, mem_id);
        current_->Remove(m);
        assert(m->file_number_ > 0);

        if (m->Unref() != nullptr) {
          to_delete->push_back(m);
        }
      } else {
        //commit failed. setup state so that we can flush again.
        LogToBuffer(log_buffer, "Level-0 commit table #%" PRIu64
                                ": memtable #%" PRIu64 " failed",
                    m->file_number_, mem_id);
        m->flush_completed_ = false;
        m->flush_in_progress_ = false;
        m->edit_.Clear();
        num_flush_not_started_++;
        m->file_number_ = 0;
        imm_flush_needed.store(true, std::memory_order_release);
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
    imm_flush_needed.store(true, std::memory_order_release);
  }
}

// Returns an estimate of the number of bytes of data in use.
size_t MemTableList::ApproximateMemoryUsage() {
  size_t total_size = 0;
  for (auto& memtable : current_->memlist_) {
    total_size += memtable->ApproximateMemoryUsage();
  }
  return total_size;
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
