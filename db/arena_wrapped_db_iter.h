//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <string>

#include "db/db_impl/db_impl.h"
#include "db/db_iter.h"
#include "db/range_del_aggregator.h"
#include "memory/arena.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Arena;
class Version;

// A wrapper iterator which wraps DB Iterator and the arena, with which the DB
// iterator is supposed to be allocated. This class is used as an entry point of
// a iterator hierarchy whose memory can be allocated inline. In that way,
// accessing the iterator tree can be more cache friendly. It is also faster
// to allocate.
// When using the class's Iterator interface, the behavior is exactly
// the same as the inner DBIter.
class ArenaWrappedDBIter : public Iterator {
 public:
  ~ArenaWrappedDBIter() override {
    if (db_iter_ != nullptr) {
      db_iter_->~DBIter();
    } else {
      assert(false);
    }
  }

  // Get the arena to be used to allocate memory for DBIter to be wrapped,
  // as well as child iterators in it.
  virtual Arena* GetArena() { return &arena_; }

  const ReadOptions& GetReadOptions() { return read_options_; }

  // Set the internal iterator wrapped inside the DB Iterator. Usually it is
  // a merging iterator.
  virtual void SetIterUnderDBIter(InternalIterator* iter) {
    db_iter_->SetIter(iter);
  }

  void SetMemtableRangetombstoneIter(
      std::unique_ptr<TruncatedRangeDelIterator>* iter) {
    memtable_range_tombstone_iter_ = iter;
  }

  bool Valid() const override { return db_iter_->Valid(); }
  void SeekToFirst() override { db_iter_->SeekToFirst(); }
  void SeekToLast() override { db_iter_->SeekToLast(); }
  // 'target' does not contain timestamp, even if user timestamp feature is
  // enabled.
  void Seek(const Slice& target) override { db_iter_->Seek(target); }
  void SeekForPrev(const Slice& target) override {
    db_iter_->SeekForPrev(target);
  }
  void Next() override { db_iter_->Next(); }
  void Prev() override { db_iter_->Prev(); }
  Slice key() const override { return db_iter_->key(); }
  Slice value() const override { return db_iter_->value(); }
  const WideColumns& columns() const override { return db_iter_->columns(); }
  Status status() const override { return db_iter_->status(); }
  Slice timestamp() const override { return db_iter_->timestamp(); }
  bool IsBlob() const { return db_iter_->IsBlob(); }

  Status GetProperty(std::string prop_name, std::string* prop) override;

  Status Refresh() override;
  Status Refresh(const Snapshot*) override;

  bool PrepareValue() override { return db_iter_->PrepareValue(); }

  void Init(Env* env, const ReadOptions& read_options,
            const ImmutableOptions& ioptions,
            const MutableCFOptions& mutable_cf_options, const Version* version,
            const SequenceNumber& sequence,
            uint64_t max_sequential_skip_in_iterations, uint64_t version_number,
            ReadCallback* read_callback, ColumnFamilyHandleImpl* cfh,
            bool expose_blob_index, bool allow_refresh);

  // Store some parameters so we can refresh the iterator at a later point
  // with these same params
  void StoreRefreshInfo(ColumnFamilyHandleImpl* cfh,
                        ReadCallback* read_callback, bool expose_blob_index) {
    cfh_ = cfh;
    read_callback_ = read_callback;
    expose_blob_index_ = expose_blob_index;
  }

 private:
  DBIter* db_iter_ = nullptr;
  Arena arena_;
  uint64_t sv_number_;
  ColumnFamilyHandleImpl* cfh_ = nullptr;
  ReadOptions read_options_;
  ReadCallback* read_callback_;
  bool expose_blob_index_ = false;
  bool allow_refresh_ = true;
  // If this is nullptr, it means the mutable memtable does not contain range
  // tombstone when added under this DBIter.
  std::unique_ptr<TruncatedRangeDelIterator>* memtable_range_tombstone_iter_ =
      nullptr;
};

// Generate the arena wrapped iterator class.
// `cfh` is used for reneweal. If left null, renewal will not
// be supported.
ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    Env* env, const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const Version* version,
    const SequenceNumber& sequence, uint64_t max_sequential_skip_in_iterations,
    uint64_t version_number, ReadCallback* read_callback,
    ColumnFamilyHandleImpl* cfh = nullptr, bool expose_blob_index = false,
    bool allow_refresh = true);
}  // namespace ROCKSDB_NAMESPACE
