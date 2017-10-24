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
#include "db/forward_iterator.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/table_cache_request.h"
#include "db/version_set_request.h"
#include "monitoring/perf_context_imp.h"

#include "util/sync_point.h"

namespace rocksdb {
namespace async {
/////////////////////////////////////////////////////////////////
/// DBImplGetContext
DBImplGetContext::DBImplGetContext(const Callback& cb, DBImpl* db,
                                   const ReadOptions& ro,
                                   const Slice& key, std::string* value, PinnableSlice* pinnable_input,
                                   ColumnFamilyHandle* column_family,
                                   bool* value_found) :
  cb_(cb),
  db_impl_(db),
  read_options_(ro),
  key_(key),
  value_(value),
  value_found_(value_found),
  cfd_(nullptr),
  sv_(nullptr),
  sw_(db_impl_->env_, db_impl_->stats_, DB_GET),
  PERF_METER_INIT(get_from_output_files_time),
  pinnable_val_input_(nullptr) {

  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  cfd_ = cfh->cfd();

  InitPinnableSlice(pinnable_input, value);

  assert(!GetPinnable().IsPinned());

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

  InitRangeDelAggreagator(cfd_->internal_comparator(), snapshot);
  InitLookupKey(key_, snapshot);

  TEST_SYNC_POINT("DBImpl::GetImpl:3");
  TEST_SYNC_POINT("DBImpl::GetImpl:4");
}

Status DBImplGetContext::GetImpl() {
  Status s;
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.

  bool skip_memtable = (read_options_.read_tier == kPersistedTier &&
                        db_impl_->has_unpersisted_data_.load(std::memory_order_relaxed));
  bool done = false;
  if (!skip_memtable) {
    if (sv_->mem->Get(GetLookupKey(), GetPinnable().GetSelf(), &s, &merge_context_,
                      &GetRangeDel(), read_options_)) {
      done = true;
      GetPinnable().PinSelf();
      RecordTick(db_impl_->stats_, MEMTABLE_HIT);
    } else if ((s.ok() || s.IsMergeInProgress()) &&
               sv_->imm->Get(GetLookupKey(), GetPinnable().GetSelf(), &s, &merge_context_,
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
    PERF_METER_START(get_from_output_files_time);
    if (cb_) {
      CallableFactory<DBImplGetContext, Status, const Status&> fac(this);
      auto on_get_complete = fac.GetCallable<&DBImplGetContext::OnGetComplete>();
      s = VersionSetGetContext::RequestGet(on_get_complete, sv_->current,
                                           read_options_, GetLookupKey(), &GetPinnable(),
                                           &s, &merge_context_,
                                           &GetRangeDel(), value_found_);

    } else {
      s = VersionSetGetContext::Get(sv_->current,
                                    read_options_, GetLookupKey(),
                                    &GetPinnable(), &s, &merge_context_,
                                    &GetRangeDel(), value_found_);
    }
    if (s.IsIOPending()) {
      return s;
    }
  }

  return OnGetComplete(s);
}

////////////////////////////////////////////////////////////////////////////////////////////////
// DBImplNewIteratorContext
Status DBImplNewIteratorContext::CreateImpl() {
  Status s;

  if (read_options_.managed) {
    // not supported in lite version
    s = Status::InvalidArgument(
          "Managed Iterators not supported in async mode yet.");
    result_ = NewErrorIterator(s);
  } else if (read_options_.tailing) {
#ifdef ROCKSDB_LITE
    // not supported in lite version
    s = Status::InvalidArgument(
          "Managed Iterators not supported in RocksDBLite");
    result_ = NewErrorIterator(s);
#else
    sv_ = cfd_->GetReferencedSuperVersion(&db_impl_->mutex_);
    InternalIterator* fwd_iterator = nullptr;
    if (cb_) {
      async::CallableFactory<DBImplNewIteratorContext, void, const Status&, InternalIterator*>
      f(this);
      auto complete_fwd_async =
        f.GetCallable<&DBImplNewIteratorContext::CompletedForwardAsync>();
      s = ForwardIteratorAsync::Create(complete_fwd_async, db_impl_, read_options_,
                                       cfd_,
                                       &fwd_iterator, sv_);
    } else {
      fwd_iterator = ForwardIteratorAsync::Create(db_impl_, read_options_, cfd_,
                     sv_);
    }
    if (s.IsIOPending()) {
      return s;
    }
    assert(fwd_iterator != nullptr);
    result_ = NewDBIteratorAsync(
                db_impl_->env_, read_options_, *cfd_->ioptions(), cfd_->user_comparator(),
                fwd_iterator,
                kMaxSequenceNumber,
                sv_->mutable_cf_options.max_sequential_skip_in_iterations,
                sv_->version_number);
#endif
  } else {
    SequenceNumber latest_snapshot = db_impl_->versions_->LastSequence();
    sv_ = cfd_->GetReferencedSuperVersion(&db_impl_->mutex_);

    auto snapshot =
      read_options_.snapshot != nullptr
      ? reinterpret_cast<const SnapshotImpl*>(
        read_options_.snapshot)->number_
      : latest_snapshot;

    db_iter_ = NewArenaWrappedDbIteratorAsync(
                 db_impl_->env_, read_options_, *cfd_->ioptions(), cfd_->user_comparator(),
                 snapshot,
                 sv_->mutable_cf_options.max_sequential_skip_in_iterations,
                 sv_->version_number);

    Arena* arena = db_iter_->GetArena();
    ConstructBuilder(&cfd_->internal_comparator(), arena,
                     !read_options_.total_order_seek &&
                     cfd_->ioptions()->prefix_extractor != nullptr);

    // Collect iterator for mutable mem
    GetBuilder()->AddIterator(
      sv_->mem->NewIterator(read_options_, arena));
    std::unique_ptr<InternalIterator> range_del_iter;
    if (!read_options_.ignore_range_deletions) {
      range_del_iter.reset(
        sv_->mem->NewRangeTombstoneIterator(read_options_));
      s = db_iter_->GetRangeDelAggregator()->AddTombstones(std::move(
            range_del_iter));
    }

    // Collect all needed child iterators for immutable memtables
    if (s.ok()) {
      sv_->imm->AddIterators(read_options_, GetBuilder());
      if (!read_options_.ignore_range_deletions) {
        s = sv_->imm->AddRangeTombstoneIterators(read_options_, arena,
            db_iter_->GetRangeDelAggregator());
      }

      if (s.ok()) {
        // Collect iterators for files in L0 - Ln
        if (read_options_.read_tier != kMemtableTier) {
          assert(sv_->current->storage_info_.finalized_);
          current_level_ = 0;
          // This will invoke FinishBuilding
          return IterateCreateLevelIterators();
        } else {
          // Success no file iterators
          FinishBuildingIterator(s);
        }
      } else {
        // This would be error case
        FinishBuildingIterator(s);
      }
    }
    if (s.IsIOPending()) {
      return s;
    }
  }

  return OnComplete(s);
}

void DBImplNewIteratorContext::CompletedForwardAsync(const Status& status,
    InternalIterator* iterator) {
  assert(status.async());
  assert(iterator != nullptr);
  assert(sv_ != nullptr);
  assert(db_iter_ == nullptr);
  async(status);
  result_ = NewDBIteratorAsync(
              db_impl_->env_, read_options_, *cfd_->ioptions(), cfd_->user_comparator(),
              iterator,
              kMaxSequenceNumber,
              sv_->mutable_cf_options.max_sequential_skip_in_iterations,
              sv_->version_number);
  OnComplete(status);
}

Status DBImplNewIteratorContext::IterateCreateLevelIterators() {
  Status s;

  Arena* arena = db_iter_->GetArena();
  auto& storage_info = sv_->current->storage_info_;
  for (; current_level_ < storage_info.num_non_empty_levels();
       ++current_level_) {
    auto& level_brief = storage_info.LevelFilesBrief(current_level_);
    if (level_brief.num_files == 0) {
      // No files in this level
      continue;
    }
    if (current_level_ == 0) {
      current_file_ = 0;
      s = IterateCreateLevelZero();
      if (s.IsIOPending()) {
        return s;
      }
    } else {
      // This code is all sync.
      InternalIterator* two_level = sv_->current->CreateTwoLevelIterator(
                                      read_options_,
                                      db_impl_->env_options_, db_iter_->GetRangeDelAggregator(), arena,
                                      current_level_);
      assert(two_level != nullptr);
      GetBuilder()->AddIterator(two_level);
    }
  }
  FinishBuildingIterator(s);
  OnComplete(s);
  return s;
}

Status DBImplNewIteratorContext::IterateCreateLevelZero() {
  Status s;
  assert(current_level_ == 0);
  Arena* arena = db_iter_->GetArena();
  auto& storage_info = sv_->current->storage_info_;
  auto& level_brief = storage_info.LevelFilesBrief(current_level_);

  TableCache::NewIteratorCallback on_level_zero_create;
  if (cb_) {
    CallableFactory<DBImplNewIteratorContext, Status, const Status&,
                    InternalIterator*, TableReader*> f(this);
    on_level_zero_create =
      f.GetCallable<&DBImplNewIteratorContext::OnCreateLevelZeroIterator>();
  }

  // Merge all level zero files together since they may overlap
  for (; current_file_ < storage_info.LevelFilesBrief(0).num_files;
       ++current_file_) {
    const auto& file = level_brief.files[current_file_];
    InternalIterator* level_zero = nullptr;

    if (cb_) {
      s = TableCacheNewIteratorContext::RequestCreate(on_level_zero_create,
          cfd_->table_cache(), read_options_, db_impl_->env_options_,
          cfd_->internal_comparator(), file.fd, db_iter_->GetRangeDelAggregator(),
          &level_zero,
          nullptr, cfd_->internal_stats()->GetFileReadHist(0), false/* for_compaction */,
          arena,
          false /*skip_filters */, 0 /*level */);
    } else {
      s = TableCacheNewIteratorContext::Create(cfd_->table_cache(),
          read_options_, db_impl_->env_options_, cfd_->internal_comparator(), file.fd,
          db_iter_->GetRangeDelAggregator(), &level_zero, nullptr,
          cfd_->internal_stats()->GetFileReadHist(0),
          false/* for_compaction */, arena, false /*skip_filters */, 0 /*level */);
    }
    if (s.IsIOPending()) {
      return s;
    }
    // iterator is always created even on error
    // to contain the status
    assert(level_zero != nullptr);
    GetBuilder()->AddIterator(level_zero);
  }
  return s;
}

Status DBImplNewIteratorContext::OnCreateLevelZeroIterator(
  const Status& status, InternalIterator* iter, TableReader*) {
  // we are ignorining table_reader
  assert(status.async()); // We do not call this
  async(status);

  // iterator is always created even on error
  // to contain the status
  assert(iter != nullptr);
  GetBuilder()->AddIterator(iter);

  assert(current_level_ == 0);
  auto& storage_info = sv_->current->storage_info_;
  auto& level_brief = storage_info.LevelFilesBrief(current_level_);
  Status s;
  ++current_file_;
  if (current_file_ < level_brief.num_files) {
    s = IterateCreateLevelZero();
    if (s.IsIOPending()) {
      return s;
    }
  }
  // Continue with other levels
  ++current_level_;
  return IterateCreateLevelIterators();
}
} //namespace async
} // namespace rocksdb

