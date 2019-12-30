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

namespace {
ReadOptions CreateReadOptions(const ReadOptions& read_opts, char* ptr,
                              const Slice* iter_lb, const Slice* iter_ub,
                              const Slice* ts) {
  ReadOptions ret = read_opts;
  const auto copy_iter_bounds_and_ts = [&ptr, ts](const Slice* bound) {
    char* saved_addr = ptr;
    ptr += sizeof(Slice);
    char* buf = ptr;
    memcpy(ptr, bound->data(), bound->size());
    ptr += bound->size();
    memcpy(ptr, ts->data(), ts->size());
    ptr += ts->size();
    Slice* bound_and_ts =
        new (saved_addr) Slice(buf, bound->size() + ts->size());
    return bound_and_ts;
  };
  if (iter_lb) {
    ret.iterate_lower_bound = copy_iter_bounds_and_ts(iter_lb);
  }
  if (iter_ub) {
    ret.iterate_upper_bound = copy_iter_bounds_and_ts(iter_ub);
  }
  return ret;
}
}  // namespace

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

void ArenaWrappedDBIter::Init(Env* env, const ReadOptions& read_options,
                              const ImmutableCFOptions& cf_options,
                              const MutableCFOptions& mutable_cf_options,
                              const SequenceNumber& sequence,
                              uint64_t max_sequential_skip_in_iteration,
                              uint64_t version_number,
                              ReadCallback* read_callback, DBImpl* db_impl,
                              ColumnFamilyData* cfd, bool allow_blob,
                              bool allow_refresh) {
  size_t extra_bytes = 0;
  const Slice* iter_lb = read_options.iterate_lower_bound;
  const Slice* iter_ub = read_options.iterate_upper_bound;
  const Slice* ts = read_options.timestamp;
  if (ts) {
    if (iter_lb) {
      extra_bytes += (sizeof(Slice) + iter_lb->size() + ts->size());
    }
    if (iter_ub) {
      extra_bytes += (sizeof(Slice) + iter_ub->size() + ts->size());
    }
  }
  auto mem = arena_.AllocateAligned(sizeof(DBIter) + extra_bytes);
  char* ptr = mem + sizeof(DBIter);
  if (extra_bytes > 0) {
    read_options_ = CreateReadOptions(read_options, ptr, iter_lb, iter_ub, ts);
  } else {
    read_options_ = read_options;
  }
  db_iter_ =
      new (mem) DBIter(env, read_options_, cf_options, mutable_cf_options,
                       cf_options.user_comparator, nullptr, sequence, true,
                       max_sequential_skip_in_iteration, read_callback, db_impl,
                       cfd, allow_blob);
  sv_number_ = version_number;
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
  SequenceNumber latest_seq = db_impl_->GetLatestSequenceNumber();
  uint64_t cur_sv_number = cfd_->GetSuperVersionNumber();
  if (sv_number_ != cur_sv_number) {
    Env* env = db_iter_->env();
    db_iter_->~DBIter();
    arena_.~Arena();
    new (&arena_) Arena();

    SuperVersion* sv = cfd_->GetReferencedSuperVersion(db_impl_);
    if (read_callback_) {
      read_callback_->Refresh(latest_seq);
    }
    Init(env, read_options_, *(cfd_->ioptions()), sv->mutable_cf_options,
         latest_seq, sv->mutable_cf_options.max_sequential_skip_in_iterations,
         cur_sv_number, read_callback_, db_impl_, cfd_, allow_blob_,
         allow_refresh_);

    InternalIterator* internal_iter = db_impl_->NewInternalIterator(
        read_options_, cfd_, sv, &arena_, db_iter_->GetRangeDelAggregator(),
        latest_seq);
    SetIterUnderDBIter(internal_iter);
  } else {
    db_iter_->set_sequence(latest_seq);
    db_iter_->set_valid(false);
  }
  return Status::OK();
}

ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    Env* env, const ReadOptions& read_options,
    const ImmutableCFOptions& cf_options,
    const MutableCFOptions& mutable_cf_options, const SequenceNumber& sequence,
    uint64_t max_sequential_skip_in_iterations, uint64_t version_number,
    ReadCallback* read_callback, DBImpl* db_impl, ColumnFamilyData* cfd,
    bool allow_blob, bool allow_refresh) {
  ArenaWrappedDBIter* iter = new ArenaWrappedDBIter();
  iter->Init(env, read_options, cf_options, mutable_cf_options, sequence,
             max_sequential_skip_in_iterations, version_number, read_callback,
             db_impl, cfd, allow_blob, allow_refresh);
  if (db_impl != nullptr && cfd != nullptr && allow_refresh) {
    iter->StoreRefreshInfo(db_impl, cfd, read_callback, allow_blob);
  }

  return iter;
}

}  // namespace ROCKSDB_NAMESPACE
