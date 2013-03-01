// Copyright (c) 2012 Facebook.


#include "db/memtablelist.h"

#include <string>
#include "leveldb/db.h"
#include "db/memtable.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

class InternalKeyComparator;
class Mutex;
class MemTableListIterator;
class VersionSet;

using std::list;

// Increase reference count on all underling memtables
void MemTableList::RefAll() {
  for (list<MemTable*>::iterator it = memlist_.begin();
       it != memlist_.end() ; ++it) {
    (*it)->Ref();
  }
}

// Drop reference count on all underling memtables
void MemTableList::UnrefAll() {
  for (list<MemTable*>::iterator it = memlist_.begin();
       it != memlist_.end() ; ++it) {
    (*it)->Unref();
  }
}

// Returns the total number of memtables in the list
int MemTableList::size() {
  assert(num_flush_not_started_ <= size_);
  return size_;
}

// Returns true if there is at least one memtable on which flush has
// not yet started.
bool MemTableList::IsFlushPending() {
  if (num_flush_not_started_ > 0) {
    assert(imm_flush_needed.NoBarrier_Load() != nullptr);
    return true;
  }
  return false;
}

// Returns the earliest memtable that needs to be flushed.
// Returns null, if no such memtable exist.
MemTable* MemTableList::PickMemtableToFlush() {
  for (list<MemTable*>::reverse_iterator it = memlist_.rbegin();
       it != memlist_.rend(); it++) {
    MemTable* m = *it;
    if (!m->flush_in_progress_) {
      assert(!m->flush_completed_);
      num_flush_not_started_--;
      if (num_flush_not_started_ == 0) {
        imm_flush_needed.Release_Store(nullptr);
      }
      m->flush_in_progress_ = true; // flushing will start very soon
      return m;
    }
  }
  return nullptr;
}

// Record a successful flush in the manifest file
Status MemTableList::InstallMemtableFlushResults(MemTable* m,
                      VersionSet* vset, Status flushStatus,
                      port::Mutex* mu, Logger* info_log,
                      uint64_t file_number,
                      std::set<uint64_t>& pending_outputs) {
  mu->AssertHeld();
  assert(m->flush_in_progress_);
  assert(m->file_number_ == 0);

  // If the flush was not successful, then just reset state.
  // Maybe a suceeding attempt to flush will be successful.
  if (!flushStatus.ok()) {
    m->flush_in_progress_ = false;
    m->flush_completed_ = false;
    m->edit_.Clear();
    num_flush_not_started_++;
    imm_flush_needed.Release_Store((void *)1);
    pending_outputs.erase(file_number);
    return flushStatus;
  }

  // flush was sucessful
  m->flush_completed_ = true;
  m->file_number_ = file_number;

  // if some other thread is already commiting, then return
  Status s;
  if (commit_in_progress_) {
    return s;
  }

  // Only a single thread can be executing this piece of code
  commit_in_progress_ = true;

  // scan all memtables from the earliest, and commit those
  // (in that order) that have finished flushing.
  while (!memlist_.empty()) {
    m = memlist_.back(); // get the last element
    if (!m->flush_completed_) {
      break;
    }
    Log(info_log,
        "Level-0 commit table #%llu: started",
        (unsigned long long)m->file_number_);

    // this can release and reacquire the mutex.
    s = vset->LogAndApply(&m->edit_, mu);

    if (s.ok()) { // commit new state
      Log(info_log, "Level-0 commit table #%llu: done",
                     (unsigned long long)m->file_number_);
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
      Log(info_log, "Level-0 commit table #%llu: failed",
                     (unsigned long long)m->file_number_);
      m->flush_completed_ = false;
      m->flush_in_progress_ = false;
      m->edit_.Clear();
      num_flush_not_started_++;
      pending_outputs.erase(m->file_number_);
      m->file_number_ = 0;
      imm_flush_needed.Release_Store((void *)1);
      s = Status::IOError("Unable to commit flushed memtable");
      break;
    }
  }
  commit_in_progress_ = false;
  return s;
}

// New memtables are inserted at the front of the list.
void MemTableList::Add(MemTable* m) {
  assert(size_ >= num_flush_not_started_);
  size_++;
  memlist_.push_front(m);
  num_flush_not_started_++;
  if (num_flush_not_started_ == 1) {
    imm_flush_needed.Release_Store((void *)1);
  }
}

// Returns an estimate of the number of bytes of data in use.
size_t MemTableList::ApproximateMemoryUsage() {
  size_t size = 0;
  for (list<MemTable*>::iterator it = memlist_.begin();
       it != memlist_.end(); ++it) {
    size += (*it)->ApproximateMemoryUsage();
  }
  return size;
}

// Search all the memtables starting from the most recent one.
// Return the most recent value found, if any.
bool MemTableList::Get(const LookupKey& key, std::string* value, Status* s) {
  for (list<MemTable*>::iterator it = memlist_.begin();
       it != memlist_.end(); ++it) {
    if ((*it)->Get(key, value, s)) {
      return true;
    }
  }
  return false;
}

void MemTableList::GetMemTables(std::vector<MemTable*>* output) {
  for (list<MemTable*>::iterator it = memlist_.begin();
       it != memlist_.end(); ++it) {
    output->push_back(*it);
  }
}

}  // namespace leveldb
