//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/arena_wrapped_db_iter.h"

#include "memory/arena.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "util/user_comparator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

inline static SequenceNumber GetSeqNum(const DBImpl* db, const Snapshot* s) {
  if (s) {
    return s->GetSequenceNumber();
  } else {
    return db->GetLatestSequenceNumber();
  }
}

Status ArenaWrappedDBIter::GetProperty(std::string prop_name,
                                       std::string* prop) {
  if (prop_name == "rocksdb.iterator.super-version-number") {
    // First try to pass the value returned from inner iterator.
    if (!db_iter_->GetProperty(prop_name, prop).ok()) {
      *prop = std::to_string(sv_number_);
    }
    return Status::OK();
  }
  return db_iter_->GetProperty(prop_name, prop);
}

void ArenaWrappedDBIter::Init(
    Env* env, const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const Version* version,
    const SequenceNumber& sequence, uint64_t max_sequential_skip_in_iteration,
    uint64_t version_number, ReadCallback* read_callback,
    ColumnFamilyHandleImpl* cfh, bool expose_blob_index, bool allow_refresh) {
  auto mem = arena_.AllocateAligned(sizeof(DBIter));
  db_iter_ = new (mem) DBIter(
      env, read_options, ioptions, mutable_cf_options, ioptions.user_comparator,
      /* iter */ nullptr, version, sequence, true,
      max_sequential_skip_in_iteration, read_callback, cfh, expose_blob_index);
  sv_number_ = version_number;
  read_options_ = read_options;
  allow_refresh_ = allow_refresh;
  memtable_range_tombstone_iter_ = nullptr;

  if (!CheckFSFeatureSupport(env->GetFileSystem().get(),
                             FSSupportedOps::kAsyncIO)) {
    read_options_.async_io = false;
  }
}

Status ArenaWrappedDBIter::Refresh() { return Refresh(nullptr); }

Status ArenaWrappedDBIter::Refresh(const Snapshot* snapshot) {
  if (cfh_ == nullptr || !allow_refresh_) {
    return Status::NotSupported("Creating renew iterator is not allowed.");
  }
  assert(db_iter_ != nullptr);
  auto cfd = cfh_->cfd();
  auto db_impl = cfh_->db();

  // TODO(yiwu): For last_seq_same_as_publish_seq_==false, this is not the
  // correct behavior. Will be corrected automatically when we take a snapshot
  // here for the case of WritePreparedTxnDB.
  uint64_t cur_sv_number = cfd->GetSuperVersionNumber();
  // If we recreate a new internal iterator below (NewInternalIterator()),
  // we will pass in read_options_. We need to make sure it
  // has the right snapshot.
  read_options_.snapshot = snapshot;
  TEST_SYNC_POINT("ArenaWrappedDBIter::Refresh:1");
  TEST_SYNC_POINT("ArenaWrappedDBIter::Refresh:2");

  auto reinit_internal_iter = [&]() {
    Env* env = db_iter_->env();
    db_iter_->~DBIter();
    arena_.~Arena();
    new (&arena_) Arena();

    SuperVersion* sv = cfd->GetReferencedSuperVersion(db_impl);
    assert(sv->version_number >= cur_sv_number);
    SequenceNumber read_seq = GetSeqNum(db_impl, snapshot);
    if (read_callback_) {
      read_callback_->Refresh(read_seq);
    }
    Init(env, read_options_, *(cfd->ioptions()), sv->mutable_cf_options,
         sv->current, read_seq,
         sv->mutable_cf_options.max_sequential_skip_in_iterations,
         sv->version_number, read_callback_, cfh_, expose_blob_index_,
         allow_refresh_);

    InternalIterator* internal_iter = db_impl->NewInternalIterator(
        read_options_, cfd, sv, &arena_, read_seq,
        /* allow_unprepared_value */ true, /* db_iter */ this);
    SetIterUnderDBIter(internal_iter);
  };
  while (true) {
    if (sv_number_ != cur_sv_number) {
      reinit_internal_iter();
      break;
    } else {
      SequenceNumber read_seq = GetSeqNum(db_impl, snapshot);
      // Refresh range-tombstones in MemTable
      if (!read_options_.ignore_range_deletions) {
        SuperVersion* sv = cfd->GetThreadLocalSuperVersion(db_impl);
        TEST_SYNC_POINT_CALLBACK("ArenaWrappedDBIter::Refresh:SV", nullptr);
        auto t = sv->mem->NewRangeTombstoneIterator(
            read_options_, read_seq, false /* immutable_memtable */);
        if (!t || t->empty()) {
          // If memtable_range_tombstone_iter_ points to a non-empty tombstone
          // iterator, then it means sv->mem is not the memtable that
          // memtable_range_tombstone_iter_ points to, so SV must have changed
          // after the sv_number_ != cur_sv_number check above. We will fall
          // back to re-init the InternalIterator, and the tombstone iterator
          // will be freed during db_iter destruction there.
          if (memtable_range_tombstone_iter_) {
            assert(!*memtable_range_tombstone_iter_ ||
                   sv_number_ != cfd->GetSuperVersionNumber());
          }
          delete t;
        } else {  // current mutable memtable has range tombstones
          if (!memtable_range_tombstone_iter_) {
            delete t;
            db_impl->ReturnAndCleanupSuperVersion(cfd, sv);
            // The memtable under DBIter did not have range tombstone before
            // refresh.
            reinit_internal_iter();
            break;
          } else {
            delete *memtable_range_tombstone_iter_;
            *memtable_range_tombstone_iter_ = new TruncatedRangeDelIterator(
                std::unique_ptr<FragmentedRangeTombstoneIterator>(t),
                &cfd->internal_comparator(), nullptr, nullptr);
          }
        }
        db_impl->ReturnAndCleanupSuperVersion(cfd, sv);
      }
      // Check again if the latest super version number is changed
      uint64_t latest_sv_number = cfd->GetSuperVersionNumber();
      if (latest_sv_number != cur_sv_number) {
        // If the super version number is changed after refreshing,
        // fallback to Re-Init the InternalIterator
        cur_sv_number = latest_sv_number;
        continue;
      }
      db_iter_->set_sequence(read_seq);
      db_iter_->set_valid(false);
      break;
    }
  }
  return Status::OK();
}

ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    Env* env, const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const Version* version,
    const SequenceNumber& sequence, uint64_t max_sequential_skip_in_iterations,
    uint64_t version_number, ReadCallback* read_callback,
    ColumnFamilyHandleImpl* cfh, bool expose_blob_index, bool allow_refresh) {
  ArenaWrappedDBIter* iter = new ArenaWrappedDBIter();
  iter->Init(env, read_options, ioptions, mutable_cf_options, version, sequence,
             max_sequential_skip_in_iterations, version_number, read_callback,
             cfh, expose_blob_index, allow_refresh);
  if (cfh != nullptr && allow_refresh) {
    iter->StoreRefreshInfo(cfh, read_callback, expose_blob_index);
  }

  return iter;
}

}  // namespace ROCKSDB_NAMESPACE
