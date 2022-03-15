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

Status ArenaWrappedDBIter::GetProperty(std::string prop_name,
                                       std::string* prop) {
  if (prop_name == "rocksdb.iterator.super-version-number") {
    // First try to pass the value returned from inner iterator.
    if (!db_iter_->GetProperty(prop_name, prop).ok()) {
      *prop = ToString(sv_number_);
    }
    return Status::OK();
  }
  return db_iter_->GetProperty(prop_name, prop);
}

void ArenaWrappedDBIter::Init(
    Env* env, const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const Version* version,
    const SequenceNumber& sequence, uint64_t max_sequential_skip_in_iteration,
    uint64_t version_number, ReadCallback* read_callback, DBImpl* db_impl,
    ColumnFamilyData* cfd, bool expose_blob_index, bool allow_refresh) {
  auto mem = arena_.AllocateAligned(sizeof(DBIter));
  db_iter_ =
      new (mem) DBIter(env, read_options, ioptions, mutable_cf_options,
                       ioptions.user_comparator, /* iter */ nullptr, version,
                       sequence, true, max_sequential_skip_in_iteration,
                       read_callback, db_impl, cfd, expose_blob_index);
  sv_number_ = version_number;
  read_options_ = read_options;
  allow_refresh_ = allow_refresh;
}

Status ArenaWrappedDBIter::Refresh() {
  if (cfd_ == nullptr || db_impl_ == nullptr || !allow_refresh_) {
    return Status::NotSupported("Creating renew iterator is not allowed.");
  }
  assert(db_iter_ != nullptr);
  // TODO(yiwu): For last_seq_same_as_publish_seq_==false, this is not the
  // correct behavior. Will be corrected automatically when we take a snapshot
  // here for the case of WritePreparedTxnDB.
  uint64_t cur_sv_number = cfd_->GetSuperVersionNumber();
  TEST_SYNC_POINT("ArenaWrappedDBIter::Refresh:1");
  TEST_SYNC_POINT("ArenaWrappedDBIter::Refresh:2");
  while (true) {
    if (sv_number_ != cur_sv_number) {
      Env* env = db_iter_->env();
      db_iter_->~DBIter();
      arena_.~Arena();
      new (&arena_) Arena();

      SuperVersion* sv = cfd_->GetReferencedSuperVersion(db_impl_);
      SequenceNumber latest_seq = db_impl_->GetLatestSequenceNumber();
      if (read_callback_) {
        read_callback_->Refresh(latest_seq);
      }
      Init(env, read_options_, *(cfd_->ioptions()), sv->mutable_cf_options,
           sv->current, latest_seq,
           sv->mutable_cf_options.max_sequential_skip_in_iterations,
           cur_sv_number, read_callback_, db_impl_, cfd_, expose_blob_index_,
           allow_refresh_);

      InternalIterator* internal_iter = db_impl_->NewInternalIterator(
          read_options_, cfd_, sv, &arena_, db_iter_->GetRangeDelAggregator(),
          latest_seq, /* allow_unprepared_value */ true);
      SetIterUnderDBIter(internal_iter);
      break;
    } else {
      SequenceNumber latest_seq = db_impl_->GetLatestSequenceNumber();
      // Refresh range-tombstones in MemTable
      if (!read_options_.ignore_range_deletions) {
        SuperVersion* sv = cfd_->GetThreadLocalSuperVersion(db_impl_);
        ReadRangeDelAggregator* range_del_agg =
            db_iter_->GetRangeDelAggregator();
        std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter;
        range_del_iter.reset(
            sv->mem->NewRangeTombstoneIterator(read_options_, latest_seq));
        range_del_agg->AddTombstones(std::move(range_del_iter));
        cfd_->ReturnThreadLocalSuperVersion(sv);
      }
      // Refresh latest sequence number
      db_iter_->set_sequence(latest_seq);
      db_iter_->set_valid(false);
      // Check again if the latest super version number is changed
      uint64_t latest_sv_number = cfd_->GetSuperVersionNumber();
      if (latest_sv_number != cur_sv_number) {
        // If the super version number is changed after refreshing,
        // fallback to Re-Init the InternalIterator
        cur_sv_number = latest_sv_number;
        continue;
      }
      break;
    }
  }
  return Status::OK();
}

ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    Env* env, const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const Version* version,
    const SequenceNumber& sequence, uint64_t max_sequential_skip_in_iterations,
    uint64_t version_number, ReadCallback* read_callback, DBImpl* db_impl,
    ColumnFamilyData* cfd, bool expose_blob_index, bool allow_refresh) {
  ArenaWrappedDBIter* iter = new ArenaWrappedDBIter();
  iter->Init(env, read_options, ioptions, mutable_cf_options, version, sequence,
             max_sequential_skip_in_iterations, version_number, read_callback,
             db_impl, cfd, expose_blob_index, allow_refresh);
  if (db_impl != nullptr && cfd != nullptr && allow_refresh) {
    iter->StoreRefreshInfo(db_impl, cfd, read_callback, expose_blob_index);
  }

  return iter;
}

}  // namespace ROCKSDB_NAMESPACE
