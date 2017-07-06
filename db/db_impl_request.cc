//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#include "db/db_impl_request.h"
#include "db/db_impl.h"
#include "db/version_set_request.h"
#include "monitoring/perf_context_imp.h"
#include "util/sync_point.h"

namespace rocksdb {
namespace async {
/////////////////////////////////////////////////////////////////
/// DBImplGetContext
DBImplGetContext::DBImplGetContext(const Callback& cb, DB* db, const ReadOptions& ro,
                           const Slice& key, std::string* value, PinnableSlice* pinnable_input,
                           ColumnFamilyHandle* column_family,
                           bool* value_found) :
  cb_(cb),
  db_impl_(reinterpret_cast<DBImpl*>(db)),
  read_options_(ro),
  key_(key),
  value_(value),
  pinnable_val_input_(nullptr),
  value_found_(value_found),
  cfd_(nullptr),
  sv_(nullptr),
  sw_(db_impl_->env_, db_impl_->stats_, DB_GET),
  PERF_TIMER_INIT(get_from_output_files_time) {

  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  cfd_ = cfh->cfd();

  InitPinnableSlice(pinnable_input, value);

  assert(!GetPinnable().IsPinned());
}

Status DBImplGetContext::GetImpl() {

  PERF_TIMER_GUARD(get_snapshot_time);

  // Acquire SuperVersion
  sv_ = db_impl_->GetAndRefSuperVersion(cfd_);

  TEST_SYNC_POINT("DBImpl::GetImpl:1");
  TEST_SYNC_POINT("DBImpl::GetImpl:2");

  SequenceNumber snapshot;
  if (read_options_.snapshot != nullptr) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(
      read_options_.snapshot)->number_;
  } else {
    // Since we get and reference the super version before getting
    // the snapshot number, without a mutex protection, it is possible
    // that a memtable switch happened in the middle and not all the
    // data for this snapshot is available. But it will contain all
    // the data available in the super version we have, which is also
    // a valid snapshot to read from.
    // We shouldn't get snapshot before finding and referencing the
    // super versipon because a flush happening in between may compact
    // away data for the snapshot, but the snapshot is earlier than the
    // data overwriting it, so users may see wrong results.
    snapshot = db_impl_->versions_->LastSequence();
  }
  TEST_SYNC_POINT("DBImpl::GetImpl:3");
  TEST_SYNC_POINT("DBImpl::GetImpl:4");

  InitRangeDelAggreagator(cfd_->internal_comparator(), snapshot);

  Status s;
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  LookupKey lkey(key_, snapshot);
  PERF_TIMER_STOP(get_snapshot_time);

  TEST_SYNC_POINT("DBImpl::GetImpl:3");
  TEST_SYNC_POINT("DBImpl::GetImpl:4");

  bool skip_memtable = (read_options_.read_tier == kPersistedTier &&
    db_impl_->has_unpersisted_data_.load(std::memory_order_relaxed));
  bool done = false;
  if (!skip_memtable) {
    if (sv_->mem->Get(lkey, GetPinnable().GetSelf(), &s, &merge_context_,
      &GetRangeDel(), read_options_)) {
      done = true;
      GetPinnable().PinSelf();
      RecordTick(db_impl_->stats_, MEMTABLE_HIT);
    } else if ((s.ok() || s.IsMergeInProgress()) &&
      sv_->imm->Get(lkey, GetPinnable().GetSelf(), &s, &merge_context_,
        &GetRangeDel(), read_options_)) {
      done = true;
      GetPinnable().PinSelf();
      RecordTick(db_impl_->stats_, MEMTABLE_HIT);
    }
    if (!done && !s.ok() && !s.IsMergeInProgress()) {
      return OnComplete(s);
    }
  }

  if (!done) {
    PERF_TIMER_START(get_from_output_files_time);
    if (cb_) {
      CallableFactory<DBImplGetContext, Status, const Status&> fac(this);
      auto on_get_complete = fac.GetCallable<&DBImplGetContext::OnGetComplete>();
      s = VersionSetGetContext::RequestGet(on_get_complete, sv_->current,
        read_options_, lkey, &GetPinnable(), &s, &merge_context_, &GetRangeDel(), value_found_);

    } else {
      s = VersionSetGetContext::Get(sv_->current,
        read_options_, lkey, &GetPinnable(), &s, &merge_context_, &GetRangeDel(), value_found_);
    }

    if (s.IsIOPending()) {
      return s;
    }
    PERF_TIMER_STOP(get_from_output_files_time);
  }

  return OnGetComplete(s);
}

Status DBImplGetContext::OnGetComplete(const Status& status) {
  async(status);

  RecordTick(db_impl_->stats_, MEMTABLE_MISS);

  if (status.async()) {
    // We are aonly async when read took place
    PERF_TIMER_STOP(get_from_output_files_time);
  }

  {
    PERF_TIMER_GUARD(get_post_process_time);

    assert(sv_);

    db_impl_->ReturnAndCleanupSuperVersion(cfd_, sv_);

    RecordTick(db_impl_->stats_, NUMBER_KEYS_READ);
    size_t size = GetPinnable().size();
    RecordTick(db_impl_->stats_, BYTES_READ, size);
    MeasureTime(db_impl_->stats_, BYTES_PER_READ, size);
  }

  return OnComplete(status);
}

Status DBImplGetContext::OnComplete(const Status& status) {

  sw_.elapse_and_disarm();

  // Do this only if we use our own pinnable
  // Otherwise this will be done by a sync
  // entry point
  if (!pinnable_val_input_) {
    if (status.ok() && GetPinnable().IsPinned()) {
      value_->assign(GetPinnable().data(), GetPinnable().size());
    }  // else value is already assigned
  }

  if (cb_ && async()) {
    ROCKS_LOG_DEBUG(
      db_impl_->immutable_db_options_.info_log.get(),
      "DBImplGetContext async completion: %s",
      status.ToString().c_str());

    Status s(status);
    s.async(true);
    cb_.Invoke(s);
    delete this;
    return status;
  }

  ROCKS_LOG_DEBUG(
    db_impl_->immutable_db_options_.info_log.get(),
    "DBImplGetContext sync completion: %s",
    status.ToString().c_str());

  return status;
}

}
}

