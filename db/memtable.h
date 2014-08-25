//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <string>
#include <memory>
#include <deque>
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "db/version_edit.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "util/arena.h"
#include "util/dynamic_bloom.h"

namespace rocksdb {

class Arena;
class Mutex;
class MemTableIterator;
class MergeContext;

class MemTable {
 public:
  struct KeyComparator : public MemTableRep::KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    virtual int operator()(const char* prefix_len_key1,
                           const char* prefix_len_key2) const;
    virtual int operator()(const char* prefix_len_key,
                           const Slice& key) const override;
  };

  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTable(const InternalKeyComparator& comparator,
                    const Options& options);

  ~MemTable();

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.
  // If the refcount goes to zero return this memtable, otherwise return null
  MemTable* Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      return this;
    }
    return nullptr;
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  size_t ApproximateMemoryUsage();

  // This method heuristically determines if the memtable should continue to
  // host more data.
  bool ShouldFlush() const { return should_flush_; }

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/dbformat.{h,cc} module.
  //
  // By default, it returns an iterator for prefix seek if prefix_extractor
  // is configured in Options.
  // arena: If not null, the arena needs to be used to allocate the Iterator.
  //        Calling ~Iterator of the iterator will destroy all the states but
  //        those allocated in arena.
  Iterator* NewIterator(const ReadOptions& options,
                        Arena* arena = nullptr);

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type,
           const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // If memtable contains Merge operation as the most recent entry for a key,
  //   and the merge process does not stop (not reaching a value or delete),
  //   prepend the current merge operand to *operands.
  //   store MergeInProgress in s, and return false.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s,
           MergeContext& merge_context, const Options& options);

  // Attempts to update the new_value inplace, else does normal Add
  // Pseudocode
  //   if key exists in current memtable && prev_value is of type kTypeValue
  //     if new sizeof(new_value) <= sizeof(prev_value)
  //       update inplace
  //     else add(key, new_value)
  //   else add(key, new_value)
  void Update(SequenceNumber seq,
              const Slice& key,
              const Slice& value);

  // If prev_value for key exits, attempts to update it inplace.
  // else returns false
  // Pseudocode
  //   if key exists in current memtable && prev_value is of type kTypeValue
  //     new_value = delta(prev_value)
  //     if sizeof(new_value) <= sizeof(prev_value)
  //       update inplace
  //     else add(key, new_value)
  //   else return false
  bool UpdateCallback(SequenceNumber seq,
                      const Slice& key,
                      const Slice& delta,
                      const Options& options);

  // Returns the number of successive merge entries starting from the newest
  // entry for the key up to the last non-merge entry or last entry for the
  // key in the memtable.
  size_t CountSuccessiveMergeEntries(const LookupKey& key);

  // Get total number of entries in the mem table.
  uint64_t GetNumEntries() const { return num_entries_; }

  // Returns the edits area that is needed for flushing the memtable
  VersionEdit* GetEdits() { return &edit_; }

  // Returns the sequence number of the first element that was inserted
  // into the memtable
  SequenceNumber GetFirstSequenceNumber() { return first_seqno_; }

  // Returns the next active logfile number when this memtable is about to
  // be flushed to storage
  uint64_t GetNextLogNumber() { return mem_next_logfile_number_; }

  // Sets the next active logfile number when this memtable is about to
  // be flushed to storage
  void SetNextLogNumber(uint64_t num) { mem_next_logfile_number_ = num; }

  // Notify the underlying storage that no more items will be added
  void MarkImmutable() { table_->MarkReadOnly(); }

  // return true if the current MemTableRep supports merge operator.
  bool IsMergeOperatorSupported() const {
    return table_->IsMergeOperatorSupported();
  }

  // return true if the current MemTableRep supports snapshots.
  bool IsSnapshotSupported() const { return table_->IsSnapshotSupported(); }

  // Get the lock associated for the key
  port::RWMutex* GetLock(const Slice& key);

  const InternalKeyComparator& GetInternalKeyComparator() const {
    return comparator_.comparator;
  }

  const Arena& TEST_GetArena() const { return arena_; }

 private:
  // Dynamically check if we can add more incoming entries.
  bool ShouldFlushNow() const;

  friend class MemTableIterator;
  friend class MemTableBackwardIterator;
  friend class MemTableList;

  KeyComparator comparator_;
  int refs_;
  const size_t kArenaBlockSize;
  const size_t kWriteBufferSize;
  Arena arena_;
  unique_ptr<MemTableRep> table_;

  uint64_t num_entries_;

  // These are used to manage memtable flushes to storage
  bool flush_in_progress_; // started the flush
  bool flush_completed_;   // finished the flush
  uint64_t file_number_;    // filled up after flush is complete

  // The updates to be applied to the transaction log when this
  // memtable is flushed to storage.
  VersionEdit edit_;

  // The sequence number of the kv that was inserted first
  SequenceNumber first_seqno_;

  // The log files earlier than this number can be deleted.
  uint64_t mem_next_logfile_number_;

  // rw locks for inplace updates
  std::vector<port::RWMutex> locks_;

  // No copying allowed
  MemTable(const MemTable&);
  void operator=(const MemTable&);

  const SliceTransform* const prefix_extractor_;
  std::unique_ptr<DynamicBloom> prefix_bloom_;

  // a flag indicating if a memtable has met the criteria to flush
  bool should_flush_;
};

extern const char* EncodeKey(std::string* scratch, const Slice& target);

}  // namespace rocksdb
