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

#pragma once

#include "async/async_status_capture.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/merge_context.h"
#include "db/range_del_aggregator.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/async/callables.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "table/merging_iterator.h"

#include "util/stop_watch.h"


#include <string>
#include <type_traits>

namespace rocksdb {

class ColumnFamilyData;
class DB;
class DBImpl;
class InternalKeyComparator;
struct ReadOptions;
struct SuperVersion;

namespace async {

namespace db_impl_request_details {
template<typename U, typename B>
inline U* SafeCast(B* b) {
#ifdef _DEBUG
  U* result = dynamic_cast<U*>(b);
  assert(result);
#else
  U* result = reinterpret_cast<U*>(b);
#endif
  return result;
}
}

// DB::Get() async implementation for DBImpl
class DBImplGetContext : private AsyncStatusCapture {
 public:
  using
  Callback = Callable<Status, const Status&>;

  DBImplGetContext(const DBImplGetContext&) = delete;
  DBImplGetContext& operator=(const DBImplGetContext&) = delete;

  static Status Get(DB* db, const ReadOptions& read_options,
                    ColumnFamilyHandle* column_family, const Slice& key,
                    PinnableSlice* pinnable_input, std::string* value,
                    bool* value_found = nullptr) {
    using namespace db_impl_request_details;
    assert(!pinnable_input || !value);
    const Callback empty_cb;
    DBImpl* db_impl = SafeCast<DBImpl>(db);
    DBImplGetContext context(empty_cb, db_impl, read_options, key, value,
                             pinnable_input, column_family, value_found);
    return context.GetImpl();
  }

  static Status RequestGet(const Callback& cb, DB* db,
                           const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           PinnableSlice* pinnable_input, std::string* value,
                           bool* value_found = nullptr) {
    using namespace db_impl_request_details;
    assert(!pinnable_input || !value);
    DBImpl* db_impl = SafeCast<DBImpl>(db);
    std::unique_ptr<DBImplGetContext> context(new DBImplGetContext(cb, db_impl,
      read_options, key, value, pinnable_input, column_family, value_found));
    Status s = context->GetImpl();
    if (s.IsIOPending()) {
      context.release();
    }
    return s;
  }

  DBImplGetContext(const Callback& cb, DBImpl* db, const ReadOptions& ro,
                   const Slice& key, std::string* value, PinnableSlice* pinnable_input,
                   ColumnFamilyHandle* cfd,
                   bool* value_found);

  ~DBImplGetContext() {
    ReturnSuperVersion();
    GetLookupKey().~LookupKey();
    GetRangeDel().~RangeDelAggregator();
    DestroyPinnableSlice();
  }

 private:

  void InitRangeDelAggreagator(const InternalKeyComparator& icomp,
                               SequenceNumber snapshot) {
    new (&range_del_agg_) RangeDelAggregator(icomp, snapshot);
  }

  RangeDelAggregator& GetRangeDel() {
    return *reinterpret_cast<RangeDelAggregator*>(&range_del_agg_);
  }

  void InitPinnableSlice(PinnableSlice* pinnable_input, std::string* value) {
    if (pinnable_input) {
      pinnable_val_input_ = pinnable_input;
    } else {
      assert(value);
      new (&pinnable_val_) PinnableSlice(value);
    }
  }

  void DestroyPinnableSlice() {
    if (!pinnable_val_input_) {
      reinterpret_cast<PinnableSlice*>(&pinnable_val_)->~PinnableSlice();
    }
  }

  PinnableSlice& GetPinnable() {
    if (pinnable_val_input_) {
      return *pinnable_val_input_;
    }
    return *reinterpret_cast<PinnableSlice*>(&pinnable_val_);
  }

  void InitLookupKey(const Slice& key, SequenceNumber snapshot) {
    new (&lookup_key_) LookupKey(key, snapshot);
  }

  const LookupKey& GetLookupKey() const {
    return *reinterpret_cast<const LookupKey*>(&lookup_key_);
  }

  void ReturnSuperVersion() {
    if (sv_) {
      db_impl_->ReturnAndCleanupSuperVersion(cfd_, sv_);
      sv_ = nullptr;
    }
  }

  Status GetImpl();

  Status OnGetComplete(const Status& status) {
    async(status);
    RecordTick(db_impl_->stats_, MEMTABLE_MISS);
    PERF_METER_STOP(get_from_output_files_time);
    {
      PERF_TIMER_GUARD(get_post_process_time);
      assert(sv_);
      RecordTick(db_impl_->stats_, NUMBER_KEYS_READ);
      size_t size = GetPinnable().size();
      RecordTick(db_impl_->stats_, BYTES_READ, size);
      MeasureTime(db_impl_->stats_, BYTES_PER_READ, size);
    }
    return OnComplete(status);
  }

  Status OnComplete(const Status& status) {
    sw_.elapse_and_disarm();
    ReturnSuperVersion();
    // Do this only if we use our own pinnable
    // Otherwise this will be done by a sync
    // entry point
    if (!pinnable_val_input_) {
      if (status.ok() && GetPinnable().IsPinned()) {
        value_->assign(GetPinnable().data(), GetPinnable().size());
      }  // else value is already assigned
    }

    if (cb_ && async()) {
      Status s(status);
      s.async(true);
      cb_.Invoke(s);
      delete this;
      return status;
    }
    return status;
  }

  Callback            cb_;
  DBImpl*             db_impl_;
  ReadOptions         read_options_;
  Slice               key_;
  std::string*        value_;
  bool*               value_found_;

  ColumnFamilyData*   cfd_;
  SuperVersion*       sv_;
  StopWatch           sw_;
  PERF_METER_DECL(get_from_output_files_time);
  MergeContext        merge_context_;
  PinnableSlice*      pinnable_val_input_; // External for sync
  std::aligned_storage<sizeof(PinnableSlice)>::type pinnable_val_;
  std::aligned_storage<sizeof(RangeDelAggregator)>::type range_del_agg_;
  std::aligned_storage<sizeof(LookupKey)>::type lookup_key_;
};

class DBImplNewIteratorContext : private async::AsyncStatusCapture {
 public:
  using
  Callback = Callable<Status, Iterator*>;

  DBImplNewIteratorContext(const DBImplNewIteratorContext&) = delete;
  DBImplNewIteratorContext& operator=(const DBImplNewIteratorContext&) = delete;

  static Iterator* Create(DB* db, const ReadOptions& read_options,
                          ColumnFamilyHandle* column_family) {
    using namespace db_impl_request_details;
    const Callback empty_cb;
    auto* db_impl = SafeCast<DBImpl>(db);
    DBImplNewIteratorContext ctx(empty_cb, db_impl, read_options, column_family);
    ctx.CreateImpl();
    return ctx.GetResult();
  }

  static Status RequestCreate(const Callback& cb, DB* db,
                              const ReadOptions& read_options,
                              ColumnFamilyHandle* column_family, Iterator** iterator) {
    assert(iterator != nullptr);
    *iterator = nullptr;
    using namespace db_impl_request_details;
    auto* db_impl = SafeCast<DBImpl>(db);
    std::unique_ptr<DBImplNewIteratorContext> ctx(new DBImplNewIteratorContext(cb,
        db_impl, read_options, column_family));
    Status s = ctx->CreateImpl();
    if (!s.IsIOPending()) {
      *iterator = ctx->GetResult();
    } else {
      ctx.release();
    }
    return s;
  }

  ~DBImplNewIteratorContext() {
    if (db_iter_) {
      assert(result_ == nullptr);
      delete db_iter_;
    } else {
      assert(db_iter_ == nullptr);
      delete result_;
    }
    if (merge_created_) {
      GetBuilder()->~MergeIteratorBuilderAsync();
      merge_created_ = false;
    }
  }

 private:

  DBImplNewIteratorContext(const Callback& cb, DBImpl* db_impl,
                           const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family) :
    cb_(cb),
    db_impl_(db_impl),
    cfd_(nullptr),
    read_options_(read_options),
    sv_(nullptr),
    db_iter_(nullptr),
    result_(nullptr),
    merge_created_(false),
    current_level_(0),
    current_file_(0) {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
    cfd_ = cfh->cfd();
  }

  Iterator* GetResult() {
    auto result = result_;
    result_ = nullptr;
    return result;
  }

  template<class ...Args>
  void ConstructBuilder(Args... args) {
    assert(!merge_created_);
    new(&builder_) MergeIteratorBuilderAsync(args...);
    merge_created_ = true;
  }

  MergeIteratorBuilderAsync* GetBuilder() {
    assert(merge_created_);
    return reinterpret_cast<MergeIteratorBuilderAsync*>(&builder_);
  }

  Status CreateImpl();

  Status IterateCreateLevelIterators();

  Status IterateCreateLevelZero();

  Status OnCreateLevelZeroIterator(const Status&, InternalIterator*,
                                   TableReader*);

  void CompletedForwardAsync(const Status&, InternalIterator* iterator);

  void FinishBuildingIterator(const Status& status) {
    InternalIterator* internal_iter = nullptr;
    if (status.ok()) {
      internal_iter = GetBuilder()->Finish();
      using namespace db_impl_detail;
      IterState* cleanup =
        new IterState(db_impl_, &db_impl_->mutex_, sv_,
                      read_options_.background_purge_on_iterator_cleanup);
      internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);
    } else {
      // BUG: See issue raised https://github.com/facebook/rocksdb/issues/2955
      internal_iter = NewErrorInternalIterator(status);
    }
    db_iter_->SetIterUnderDBIter(internal_iter);
    result_ = db_iter_;
    db_iter_ = nullptr;
  }

  Status OnComplete(const Status& status) {
    if (cb_ && async()) {
      Status s(status);
      s.async(true);
      cb_.Invoke(GetResult());
      delete this;
      return status;
    }
    return status;
  }

  Callback            cb_;
  DBImpl*             db_impl_;
  ColumnFamilyData*   cfd_;
  ReadOptions         read_options_;
  SuperVersion*       sv_;
  ArenaWrappedDBIterAsync* db_iter_;
  Iterator*           result_;
  bool                merge_created_;
  std::aligned_storage<sizeof(MergeIteratorBuilderAsync)>::type builder_;
  int
  current_level_; // Level for which we are constructing iterators
  size_t              current_file_; //current file within zero level
};

}
}

